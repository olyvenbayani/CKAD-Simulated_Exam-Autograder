FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cnmp-simulatedexam-grader.py .
CMD ["python", "cnmp-simulatedexam-grader.py"]