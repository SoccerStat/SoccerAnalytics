.PHONY: install lint all

# Variables
PKG=soccerscraping

all: install lint

install:
	@echo "📦 Installing dependencies..."
	@python -m pip install --upgrade pip
	@pip install -r requirements.txt

lint:
	@echo "🔍 Linting with flake8..."
	@flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
	@flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics