FROM python:3.10.8

WORKDIR /root

COPY ./Dockerfile/requirements.txt ./
COPY ./secrets/ ./secrets/
RUN pip install --no-cache-dir -U pip setuptools wheel && \
    pip install --no-cache-dir -U -r requirements.txt && \
    prefect cloud login --key $(cat /root/secrets/prefect-api-key) --workspace wonjaelee2gmailcom/dev

EXPOSE 8787 8888

CMD ["jupyter", "lab", "--allow-root", "--no-browser", "--ip=0.0.0.0", "--port=8888"]