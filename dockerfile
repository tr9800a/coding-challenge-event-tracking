FROM python:3.8
WORKDIR /app
COPY requirements.txt /app
COPY vehicle_tracker.py /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
CMD ["python", "vehicle_tracker.py"]