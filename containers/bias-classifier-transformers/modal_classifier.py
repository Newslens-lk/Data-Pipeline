"""
Modal remote GPU function for HelaBERT bias classification.

Deploy once with:
    modal deploy modal_classifier.py

Then call from run.py via Modal's client API.
"""
import modal

app = modal.App("newslens-bias-classifier")

gpu_image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch", "transformers"
)


@app.cls(gpu="T4", image=gpu_image, timeout=600)
class BiasClassifier:
    @modal.enter()
    def load_model(self):
        """Runs once when the container starts — downloads and loads the model."""
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        model_uri = "models/bias-classifier-helabert"
        self.tokenizer = AutoTokenizer.from_pretrained(model_uri)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_uri).cuda().eval()
        self.device = "cuda"

    @modal.method()
    def classify(self, texts: list[str]) -> list[list[float]]:
        """Takes a batch of texts, returns per-class probabilities."""
        import torch

        inputs = self.tokenizer(
            texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
        ).to(self.device)

        with torch.no_grad():
            logits = self.model(**inputs).logits
            probs = torch.softmax(logits, dim=-1).cpu().tolist()

        return probs
