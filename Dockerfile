FROM python:3.9-alpine

COPY /scrap .

RUN apk update && apk add --no-cache \
    firefox \
    wget \
    tar

RUN GECKOVER=$(wget -qO- https://api.github.com/repos/mozilla/geckodriver/releases/latest | grep tag_name | cut -d '"' -f 4)  && \ 
    wget "https://github.com/mozilla/geckodriver/releases/download/$GECKOVER/geckodriver-$GECKOVER-linux64.tar.gz" && \
    tar -xf "geckodriver-$GECKOVER-linux64.tar.gz"  && \
    mv geckodriver /usr/bin

RUN pip install pip --upgrade && \
    pip install --no-cache-dir -r requirements.txt 

CMD ["python", "selic_scrap.py"]
