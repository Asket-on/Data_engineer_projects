import os

print(f"KAFKA_HOST: {os.getenv('KAFKA_HOST')}")

# Получение всех переменных окружения (аналогично команде `set` в bash)
# all_env_variables = os.environ
# print(f"Все переменные окружения: {all_env_variables}")