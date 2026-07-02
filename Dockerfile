FROM anaconda/miniconda

WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt
COPY sql_queries.py bot.py supervisor.py config.yaml /app/

CMD ["python", "supervisor.py"]
