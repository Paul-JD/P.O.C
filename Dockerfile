FROM python:3.9

WORKDIR /app

RUN python -m pip install --upgrade pip

COPY ["requirements.txt","./"]

RUN pip install -r requirements.txt

RUN pip install gunicorn

COPY ["Utility","predict.py","./" ]

EXPOSE 9696

ENTRYPOINT ["gunicorn","--bind=0.0.0.0:9696","predict:app"]