FROM anaconda/miniconda

COPY sql_queries.py requirements.txt bot.py config.yaml config.yaml /app/
RUN pip install -r /app/requirements.txt
WORKDIR /app

CMD ["python", "bot.py"]
