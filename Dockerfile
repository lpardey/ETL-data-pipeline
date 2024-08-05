FROM python:3.11 AS base

WORKDIR /app

# Google cloud credentials
RUN apt update -y && \
    apt install -y apt-transport-https ca-certificates gnupg curl
# Add the gcloud CLI distribution URI as a package source:
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    # Import the Google Cloud public key: 
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    # Update and install the gcloud CLI: 
    apt update -y && apt install google-cloud-cli -y

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

FROM base AS dev

COPY requirements-dev.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt -r requirements-dev.txt
