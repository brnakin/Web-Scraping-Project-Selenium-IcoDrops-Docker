FROM apache/airflow:2.10.4

USER root

# Set non-interactive mode for apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Install Chrome dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libpango-1.0-0 \
    libvulkan1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    libnss3 \
    libgconf-2-4 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libglib2.0-0 \
    libatk1.0-0 \
    libasound2 \
    libpangocairo-1.0-0 \
    libcups2 \
    libpango-1.0-0 \
    libxshmfence1 \
    fonts-liberation \
    --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /opt/chrome /opt/chromedriver /var/run/chrome

# Install specific version of Chrome (114.0.5735.90)
RUN wget -q https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1135561%2Fchrome-linux.zip?alt=media -O /tmp/chrome.zip \
    && unzip /tmp/chrome.zip -d /opt/ \
    && mv /opt/chrome-linux/* /opt/chrome/ \
    && rm -rf /opt/chrome-linux /tmp/chrome.zip

# Install corresponding ChromeDriver
RUN wget -q https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip -O /tmp/chromedriver.zip \
    && unzip /tmp/chromedriver.zip -d /opt/chromedriver/ \
    && rm /tmp/chromedriver.zip \
    && chmod +x /opt/chromedriver/chromedriver \
    && ln -s /opt/chromedriver/chromedriver /usr/local/bin/chromedriver

# Create symbolic link for Chrome
RUN ln -s /opt/chrome/chrome /usr/local/bin/chrome

# Set up Chrome environment
ENV CHROME_PATH=/opt/chrome/chrome
ENV CHROMEDRIVER_PATH=/opt/chromedriver/chromedriver

# Set proper permissions
RUN chmod 755 /opt/chrome/chrome \
    && chmod 755 /opt/chromedriver/chromedriver \
    && chown -R airflow:root /opt/chrome /opt/chromedriver /var/run/chrome

# Create required directories with proper permissions
RUN mkdir -p /tmp/.X11-unix /dev/shm \
    && chmod 1777 /tmp/.X11-unix /dev/shm

# Create the directory for data storage
RUN mkdir -p /opt/airflow/data/ico_data && chown -R airflow:root /opt/airflow/data

# Switch back to airflow user
USER airflow

# Install required Python packages
RUN pip install --no-cache-dir \
    selenium \
    pandas \
    psycopg2-binary \
    apache-airflow \
    apache-airflow-providers-google \
    numpy \
    seaborn \
    matplotlib \
    scikit-learn \
    webdriver-manager \
    jupyter

