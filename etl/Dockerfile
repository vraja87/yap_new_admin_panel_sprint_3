FROM python:3.10

WORKDIR /etl

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

COPY requirements.txt requirements.txt
COPY run_main.sh run_main.sh

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

COPY . .

# запускаем main.py через cron.
#RUN apt-get update && apt-get -y install cron
#COPY crontab /etc/cron.d/crontab
#RUN chmod 0644 /etc/cron.d/crontab
#RUN chmod 0644 /etc/cron.d/crontab
#RUN /usr/bin/crontab /etc/cron.d/crontab
#RUN touch /var/log/cron.log
#CMD ["cron", "-f"]
# не работает через крон... хоть убейся. опять через shell скрипт:

ENTRYPOINT "./run_main.sh"
