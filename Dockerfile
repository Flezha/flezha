# Use the official Elixir image as the base image
FROM elixir:1.17

# Set environment variables
ENV MIX_ENV=prod

# Create a non-root user and switch to it
RUN addgroup --system flecha && adduser --system flecha --ingroup flecha
USER flecha

# Set the working directory
WORKDIR /home/flecha

# Copy only the necessary files from the _build/prod/rel/flecha_core directory
COPY --chown=flecha:flecha _build/prod/rel/flecha_core .

# Start the application
CMD ["bin/flecha_core", "start"]