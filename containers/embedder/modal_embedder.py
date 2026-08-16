"""
Modal remote GPU function for generating embeddings.

Deploy once with:
    modal deploy modal_embedder.py

Then call from run.py via Modal's client API.
"""
import modal

app = modal.App("newslens-embedder")

# Container image that Modal builds and caches on their infra
gpu_image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch", "transformers"
)


@app.cls(gpu="T4", image=gpu_image, timeout=600)
class Embedder:
    @modal.enter()
    def load_model(self):
        """Runs once when the container starts — downloads and loads the model."""
        import torch
        from transformers import AutoModel, AutoTokenizer

        model_name = "intfloat/multilingual-e5-large"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).cuda().eval()
        self.device = "cuda"

    @modal.method()
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Takes a batch of texts, returns L2-normalized embeddings."""
        import torch
        import torch.nn.functional as F

        inputs = self.tokenizer(
            texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)

        # Mean pooling
        attention_mask = inputs["attention_mask"]
        token_embeddings = outputs.last_hidden_state
        mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        sum_embeddings = torch.sum(token_embeddings * mask_expanded, dim=1)
        sum_mask = torch.clamp(mask_expanded.sum(dim=1), min=1e-9)
        embeddings = sum_embeddings / sum_mask

        # Normalize
        embeddings = F.normalize(embeddings, p=2, dim=1)

        return embeddings.cpu().tolist()
