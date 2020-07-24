FROM python:3.8

# Make a user for wallpaper and use it by default
RUN groupadd --gid 946 -r wallpaper \
    && useradd --uid 946 --no-log-init -r -g wallpaper wallpaper

# Mark that the app should work in the context of the app
WORKDIR /app

RUN chown wallpaper.wallpaper /app

COPY --chown=wallpaper:wallpaper requirements.txt requirements.txt

# Setup the python environment
RUN python3 -m pip install -r requirements.txt

COPY --chown=wallpaper:wallpaper . .

# Pivot to our constrained user
USER wallpaper

# Load the example configs as actual configs so people can launch the container
# enough to get the config files.
RUN cp wallpaper_config.yaml.example wallpaper_config.yaml \
    && cp auth_info.yaml.example auth_info.yaml

# Configure docker to launch our program
CMD ["/app/wallpaperwatcher.py", "-v"]
