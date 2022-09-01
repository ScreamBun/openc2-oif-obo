FROM python:3

ADD requirements.txt /

RUN pip install -r requirements.txt

EXPOSE 8880:8880

ADD main.py /

CMD [ "python", "./main.py" ]