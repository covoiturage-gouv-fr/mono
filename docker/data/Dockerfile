FROM python:3.13.6-slim-bullseye AS build

WORKDIR /usr/src/

COPY ./notebooks/src/*.ipynb /usr/src/

RUN pip install jupyter nbconvert

RUN jupyter nbconvert --to script /usr/src/*.ipynb

RUN rm /usr/src/*.ipynb

FROM python:3.13.6-slim-bullseye AS run

WORKDIR /usr/src/

COPY --from=build /usr/src /usr/src

COPY ./notebooks/requirements.txt /usr/src/
COPY ./notebooks/src/helpers/ /usr/src/helpers/

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]

CMD ["python", "--version"]