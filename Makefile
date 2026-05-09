.PHONY: help submodule-init

help:
	@echo "Available commands:"
	@echo "  make submodule-init   - Initialize the ai-rules git submodule"
	@echo ""
	@echo "Per-subproject installs are intentionally not in this Makefile —"
	@echo "  cd <subproject> && poetry install"
	@echo ""
	@echo "See docs/getting-started.md for the full setup guide."

submodule-init:
	git submodule update --init --recursive
	@echo "ai-rules submodule initialized at: $$(cd ai-rules && git rev-parse --short HEAD)"
