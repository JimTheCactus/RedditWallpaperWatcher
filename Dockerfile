FROM python:3.9

# Mark that the app should work in the context of the app
WORKDIR /app

# Copy our prereqs in.
COPY requirements.txt requirements.txt

# Setup the python environment
RUN python3 -m pip install -r requirements.txt

# Copy the app in.
COPY . .

# Load the example configs as actual configs so people can launch the container
# enough to get the config files.
RUN cp wallpaper_config.yaml.example wallpaper_config.yaml \
    && cp auth_info.yaml.example auth_info.yaml

# Configure docker to launch our program
CMD ["/app/wallpaperwatcher.py", "-v"]
