FROM python:3.10-alpine

COPY /scrap/selic_scrapper/ .

RUN apk add --no-cache \
    firefox \
    geckodriver

ENV DELIMITER='\t'

RUN pip install --no-cache-dir -r requirements.txt 

CMD ["python", "selic_scrapper.py"]
