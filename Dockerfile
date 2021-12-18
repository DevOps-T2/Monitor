FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

COPY ./.env /code/app/.env

WORKDIR /code/app

CMD ["uvicorn", "main:app", "--root-path", "/api/monitor/", "--proxy-headers", "--host", "0.0.0.0", "--port", "80"]

EXPOSE 80