FROM python:3.11-alpine

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

ENV PYTHONPATH="${PYTHONPATH}:./app"

# Set the command to run the Hatchet scheduler
CMD ["python", "-m", "app.main"]