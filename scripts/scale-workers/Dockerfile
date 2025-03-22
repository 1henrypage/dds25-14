FROM alpine:latest

# Install curl & Docker CLI (for scaling)
RUN apk --no-cache add curl docker-cli bash

# Copy the script
COPY scale-workers.sh /scale-workers.sh
RUN chmod +x /scale-workers.sh

# Run the script
CMD ["/bin/bash", "/scale-workers.sh"]
