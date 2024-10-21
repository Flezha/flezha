# Stage 1: Build the app
FROM elixir:1.17 AS build

# Set current working directory.
WORKDIR /app

# Install system dependencies
RUN mix local.hex --force
RUN mix local.rebar --force

# Copy the mix.exs and mix.lock files
COPY mix.exs mix.lock ./

# Fetch dependencies
RUN mix deps.get

# Compile the project
RUN MIX_ENV=prod mix compile

# Copy the remaining files
COPY . .

# Create release
RUN MIX_ENV=prod mix release --overwrite

# Stage 2: Final image
FROM elixir:1.17
WORKDIR /flecha

# Set environment variables
ENV MIX_ENV=prod

# Create a non-root user and switch to it
RUN addgroup --system flecha && adduser --system flecha --ingroup flecha

# Copy only the necessary files from the _build/prod/rel/flecha_core directory
COPY --from=build /app/_build/prod/rel/flecha_core /flecha/

# Add all files under flecha permissions
RUN chown -R flecha:flecha /flecha

# Switch to the flecha user
USER flecha

# Set the working directory
WORKDIR /flecha

# Start the application
CMD ["bin/flecha_core", "start"]