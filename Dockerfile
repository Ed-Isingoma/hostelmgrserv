FROM node:20

# Set app directory
WORKDIR /app

# Copy dependency files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy rest of the app
COPY . .

# Expose the port your app runs on (adjust if needed)
EXPOSE 3000

# Start the app
CMD ["node", "main.js"]
