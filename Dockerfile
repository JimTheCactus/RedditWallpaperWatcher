FROM python:3.8
COPY . /app
WORKDIR /app
RUN python3 -m pip install -r requirements.txt
CMD ["python3", "wallpaperwatcher.py"]
