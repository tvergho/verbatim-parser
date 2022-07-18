FROM --platform=linux/amd64 python:3.8-slim

WORKDIR /app
RUN python3 -m venv /app/env

COPY ./requirements.txt /app/requirements.txt
RUN . /app/env/bin/activate
RUN pip install -r requirements.txt

COPY ./api.py /app
COPY ./search.py /app
COPY ./prod.py /app
COPY .env /app

ENV FLASK_APP=api
ENV PORT=5000

EXPOSE 5000

CMD ["python3", "prod.py"]