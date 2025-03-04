FROM python:3.12-slim

WORKDIR /home/quart-app

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

# Add /home to the PYTHONPATH so that Python can locate the 'common' module
ENV PYTHONPATH="${PYTHONPATH}:/home"

EXPOSE 5000
