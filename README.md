```bash
cp .env.example .env
docker compose -f docker/docker-compose.yml up --build -d
```
- swagger: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672
- PostgreSQL: `localhost:5432`
- Default API key: `local-development-key`
