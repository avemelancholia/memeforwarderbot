FROM anaconda/miniconda

WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt
COPY sql_queries.py bot.py config.yaml /app/

CMD ["python", "bot.py"]
