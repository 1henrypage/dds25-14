FROM python:3.12-slim

WORKDIR /home/worker

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

# Add /home to the PYTHONPATH so that Python can locate the 'common' module
ENV PYTHONPATH="${PYTHONPATH}:/home"

CMD ["python", "./worker.py"]