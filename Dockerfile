FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir --upgrade -r requirements.txt
EXPOSE 8080
CMD ["python", "app.py"]