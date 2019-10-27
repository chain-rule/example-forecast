all:

training-preprocessing:
	python -m assistance.main \
		--action preprocessing \
		--config configs/training/preprocessing.json

.PHONY: all training-preprocessing
