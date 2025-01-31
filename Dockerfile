FROM python:3.13.1-bookworm 
WORKDIR /application
#install rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "PATH=$PATH:/root/.cargo/bin" >>/etc/profile

WORKDIR /application
COPY ./reveal /application/reveal
COPY main.py /application/main.py
COPY requirements.txt /application/requirements.txt
RUN /usr/local/bin/pip install --no-cache-dir --upgrade -r requirements.txt
CMD ["fastapi", "run","/application/main.py", "--port", "80"]
