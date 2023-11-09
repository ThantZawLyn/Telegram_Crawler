# Use your custom base image as the base image
FROM mglue/telegram-base-image:1.1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
#COPY keyword.txt .

# Copy the requirements.txt file into the container
#COPY stopword.txt .

# Copy your Python project files into the container
COPY . /app

# Copy your Python project files into the container
COPY /app.py .

# Run the application
CMD ["python", "app.py"]