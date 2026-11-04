# The runner's image: the project, and the gems `bundle cache` put in vendor/cache, on Ruby 4.0.
# msgpack builds a C extension when it is installed, so the gems are installed in the full Ruby
# image, which has a compiler, and copied onto the slim one.
FROM ruby:4.0 AS gems
WORKDIR /agentdesk
COPY Gemfile Gemfile.lock ./
COPY vendor/cache vendor/cache
RUN bundle install --local

FROM ruby:4.0-slim
COPY --from=gems /usr/local/bundle /usr/local/bundle
COPY . /agentdesk
WORKDIR /agentdesk
# The runner stops when its standard input closes, and a container's is closed: it reads a pipe
# that stays open instead.
RUN mkfifo -m 600 stdin && chown 1000 stdin
USER 1000
ENTRYPOINT ["sh", "-c", "exec ruby runner.rb <> stdin"]
