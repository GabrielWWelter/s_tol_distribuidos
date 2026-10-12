FROM python:3.11-slim
WORKDIR /app
COPY Etapa1/ /app/
ENV PYTHONUNBUFFERED=1

# Porta padrão UDP
EXPOSE 4000/udp

CMD ["python3", "servidor.py", "4000"]
