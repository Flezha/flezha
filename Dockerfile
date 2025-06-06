# Stage 1: Build the app
FROM elixir:1.17-otp-27 AS build

# Set current working directory.
WORKDIR /app

# Install system dependencies
RUN mix local.hex --force
RUN mix local.rebar --force

# Copy all the necessary files to the container
COPY . .

# Fetch dependencies
RUN MIX_ENV=prod mix deps.get

# Compile the project
RUN ERL_FLAGS="+JMsingle true" MIX_ENV=prod mix compile

# Create release
RUN MIX_ENV=prod mix release --overwrite

# Stage 2: Final image
FROM elixir:1.17-otp-27

# Set the working directory
WORKDIR /flezha

# Set environment variables
ENV MIX_ENV=prod

# Create a non-root user and switch to it
RUN addgroup --system flezha && adduser --system flezha --ingroup flezha

# Copy only the necessary files from the _build/prod/rel/flezha directory
COPY --from=build /app/_build/prod/rel/flezha /flezha/

# Change the owner of the /flezha directory
RUN chown -R flezha:flezha /flezha

# Switch to the flezha user
USER flezha

# Set the working directory
WORKDIR /flezha

# Start the application
CMD ["bin/flezha", "start"]