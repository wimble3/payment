FROM python:3.13-slim
RUN python -m pip install --no-cache-dir --disable-pip-version-check "uv==0.9.8"
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PATH="/service/.venv/bin:$PATH"
WORKDIR /service
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev
COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
RUN useradd --create-home service
USER service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
