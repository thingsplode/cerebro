Run the webui
```bash
brew install ollama
ollama pull llama3.1:8b
ollama serve
ollama list
ollama show llama3.1:8b
ollama run llama3.1:8b
docker run -d -p 3000:8080 --add-host=host.docker.internal:host-gateway -v open-webui:/app/backend/data --name open-webui --restart always ghcr.io/open-webui/open-webui:main
```


