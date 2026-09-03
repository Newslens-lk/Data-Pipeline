"""
Modal remote GPU function for HelaBERT bias classification.

Deploy once with:
    modal deploy modal_classifier.py

Then call from run.py via Modal's client API.
"""
import modal

app = modal.App("newslens-bias-classifier")

REPO_ID = "sychpra/helabert-bias-classifier"
MAX_LENGTH = 512
NUM_LABELS = 5

gpu_image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch", "transformers", "sentencepiece", "huggingface_hub"
)


@app.cls(gpu="T4", image=gpu_image, timeout=600)
class BiasClassifier:
    @modal.enter()
    def load_model(self):
        """Runs once when the container starts — downloads and loads the model."""
        import torch
        import torch.nn as nn
        from huggingface_hub import hf_hub_download
        from transformers import BertModel

        # Download files from HF Hub
        bert_dir = hf_hub_download(REPO_ID, "bert/config.json")
        bert_dir = bert_dir.rsplit("/", 1)[0]  # get directory path
        # Ensure the model weights are downloaded too
        hf_hub_download(REPO_ID, "bert/model.safetensors")
        sp_path = hf_hub_download(REPO_ID, "tokenizer/unigram_32000_0.9995.model")
        head_path = hf_hub_download(REPO_ID, "classifier_head.pt")

        # Load SentencePiece tokenizer
        import sentencepiece as spm
        self.sp = spm.SentencePieceProcessor()
        self.sp.Load(sp_path)

        # Load BERT encoder
        bert = BertModel.from_pretrained(bert_dir)

        # Build full model
        class HelaBERTClassifier(nn.Module):
            def __init__(self, bert, num_labels=NUM_LABELS):
                super().__init__()
                self.bert = bert
                self.dropout = nn.Dropout(0.1)
                self.classifier = nn.Linear(768, num_labels)

            def forward(self, input_ids, attention_mask):
                outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
                hidden_states = outputs.last_hidden_state
                mask = attention_mask.unsqueeze(-1).float()
                pooled = (hidden_states * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1e-9)
                pooled = self.dropout(pooled)
                return self.classifier(pooled)

        self.model = HelaBERTClassifier(bert)
        self.model.classifier.load_state_dict(torch.load(head_path, map_location="cpu"))
        self.model.cuda().eval()
        self.device = "cuda"

    def _tokenize(self, texts: list[str]) -> tuple:
        """Tokenize texts using SentencePiece, matching training preprocessing."""
        import torch

        all_ids = []
        all_masks = []
        for text in texts:
            ids = self.sp.encode(text, out_type=int)
            ids = ids[:MAX_LENGTH]
            mask = [1] * len(ids)
            pad_len = MAX_LENGTH - len(ids)
            ids += [self.sp.pad_id()] * pad_len
            mask += [0] * pad_len
            all_ids.append(ids)
            all_masks.append(mask)

        return (
            torch.tensor(all_ids, dtype=torch.long).to(self.device),
            torch.tensor(all_masks, dtype=torch.long).to(self.device),
        )

    @modal.method()
    def classify(self, texts: list[str]) -> list[list[float]]:
        """Takes a batch of texts, returns per-class probabilities."""
        import torch

        input_ids, attention_mask = self._tokenize(texts)

        with torch.no_grad():
            logits = self.model(input_ids, attention_mask)
            probs = torch.softmax(logits, dim=-1).cpu().tolist()

        return probs
