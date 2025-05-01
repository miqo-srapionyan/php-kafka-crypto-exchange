FROM php:8.2-cli

# Install dependencies and librdkafka
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    libz-dev \
    libpq-dev \
    git \
    unzip \
    && docker-php-ext-install sockets \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka

WORKDIR /var/www/html

