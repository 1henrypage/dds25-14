FROM alpine:latest

# Install curl & Docker CLI (for scaling)
RUN apk --no-cache add curl docker-cli bash openssl

# Copy the script
COPY ./scripts/scale-workers/scale-workers.sh /scale-workers.sh
RUN chmod +x /scale-workers.sh

# Copy environment files to the container
COPY env/order.env /env/order.env
COPY env/payment.env /env/payment.env
COPY env/stock.env /env/stock.env

# Run the script
CMD ["/bin/bash", "/scale-workers.sh"]
