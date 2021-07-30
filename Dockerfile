FROM python:3.7
RUN mkdir /code
RUN apt-get update && apt-get -y install cron
WORKDIR /code
ADD requirements.txt /code/
RUN pip install -r requirements.txt
ADD . /code/
COPY src/.env /.env
RUN chmod 0744 /code/training.sh
COPY entrypoint.sh /code/entrypoint.sh
RUN chmod +x /code/entrypoint.sh
ENTRYPOINT "/code/entrypoint.sh"
