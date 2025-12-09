# Deploying CiteConnect to Google Compute Engine VM

Complete guide for deploying your Airflow pipeline to a GCE VM instance.

---

## 📋 **Prerequisites**

1. **GCP Project with Billing Enabled**
   ```bash
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Enable Required APIs**
   ```bash
   gcloud services enable compute.googleapis.com
   gcloud services enable storage-component.googleapis.com
   ```

3. **Set Up Firewall Rules** (for Airflow UI access)
   ```bash
   # Allow HTTP traffic
   gcloud compute firewall-rules create allow-http \
       --allow tcp:8080 \
       --source-ranges 0.0.0.0/0 \
       --description "Allow HTTP traffic to Airflow"
   
   # Allow HTTPS traffic (optional, for secure access)
   gcloud compute firewall-rules create allow-https \
       --allow tcp:443 \
       --source-ranges 0.0.0.0/0 \
       --description "Allow HTTPS traffic"
   ```

---

## 🚀 **Step 1: Create Compute Engine VM**

### **Option A: Using gcloud CLI (Recommended)**

```bash
# Create VM instance
gcloud compute instances create citeconnect-vm \
    --zone=us-central1-a \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --boot-disk-type=pd-standard \
    --tags=http-server,https-server \
    --metadata=startup-script='#!/bin/bash
        apt-get update
        apt-get install -y docker.io docker-compose git
        systemctl start docker
        systemctl enable docker
        usermod -aG docker $USER
    '
```

### **Option B: Using GCP Console**

1. Go to [Compute Engine](https://console.cloud.google.com/compute/instances)
2. Click **"Create Instance"**
3. Configure:
   - **Name:** `citeconnect-vm`
   - **Region:** `us-central1`
   - **Zone:** `us-central1-a`
   - **Machine type:** `e2-standard-4` (4 vCPUs, 16GB RAM)
   - **Boot disk:** Ubuntu 22.04 LTS, 50GB
   - **Firewall:** Allow HTTP and HTTPS traffic
4. Click **"Create"**

---

## 📦 **Step 2: Install Docker and Dependencies**

### **2.1 SSH into VM**

```bash
# SSH into the VM
gcloud compute ssh citeconnect-vm --zone=us-central1-a
```

### **2.2 Install Docker and Docker Compose**

```bash
# Update system
sudo apt-get update

# Install Docker
sudo apt-get install -y docker.io docker-compose git curl

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (to run without sudo)
sudo usermod -aG docker $USER

# Log out and back in for group changes to take effect
exit
```

SSH back in:
```bash
gcloud compute ssh citeconnect-vm --zone=us-central1-a
```

### **2.3 Verify Docker Installation**

```bash
docker --version
docker-compose --version
```

---

## 📁 **Step 3: Clone and Set Up Project**

### **3.1 Clone Repository**

```bash
# Create project directory
mkdir -p ~/citeconnect
cd ~/citeconnect

# Clone your repository (replace with your repo URL)
git clone https://github.com/YOUR_USERNAME/CiteConnect-DataPipeline.git .

# Or if you have the code locally, use gcloud to copy it:
# From your local machine:
# gcloud compute scp --recurse . citeconnect-vm:~/citeconnect --zone=us-central1-a
```

### **3.2 Set Up Environment Variables**

```bash
# Create .env file
nano .env
```

Add your environment variables (see your existing `.env` or README for reference):

```bash
# API Keys
SEMANTIC_SCHOLAR_API_KEY=your_key_here
SEMANTIC_SCHOLAR_API_KEYS=key1,key2,key3
UNPAYWALL_EMAIL=your_email@example.com
CORE_API_KEY=your_core_api_key

# Google Cloud
GCS_BUCKET_NAME=citeconnect-test-bucket
GCS_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/configs/credentials/gcs-key.json

# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_NAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# Email
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password

# Pipeline Configuration
SEARCH_TERMS=finance,quantum computing,healthcare
PAPERS_PER_TERM=100
MAX_REFERENCES_PER_PAPER=0
COLLECTION_DOMAIN=general
COLLECTION_SUBDOMAINS=
PROCESSING_MAX_WORKERS=10

# Embeddings
EMBEDDING_PROVIDER=local
SENTENCE_TRANSFORMERS_MODEL=all-MiniLM-L6-v2
LOCAL_EMBEDDINGS_PATH=/opt/airflow/working_data/embeddings_db.pkl
EMBEDDING_BATCH_SIZE=32
CHUNK_SIZE=512
CHUNK_OVERLAP=50
```

### **3.3 Set Up GCP Credentials**

```bash
# Create credentials directory
mkdir -p configs/credentials

# Upload your GCP service account key
# From your local machine:
# gcloud compute scp configs/credentials/gcs-key.json \
#     citeconnect-vm:~/citeconnect/configs/credentials/gcs-key.json \
#     --zone=us-central1-a

# Or create service account key directly on VM:
# gcloud iam service-accounts keys create \
#     configs/credentials/gcs-key.json \
#     --iam-account=your-service-account@project.iam.gserviceaccount.com
```

---

## 🐳 **Step 4: Start Docker Compose**

### **4.1 Build and Start Services**

```bash
cd ~/citeconnect

# Build and start all services
docker-compose up -d --build

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### **4.2 Wait for Services to Initialize**

Wait 2-3 minutes for Airflow to initialize, then check:

```bash
# Check if services are running
docker-compose ps

# Check Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

---

## 🌐 **Step 5: Access Airflow UI**

### **5.1 Get VM External IP**

```bash
# From your local machine
gcloud compute instances describe citeconnect-vm \
    --zone=us-central1-a \
    --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
```

Or check in GCP Console: Compute Engine → VM instances → citeconnect-vm

### **5.2 Access Airflow**

Open in browser:
```
http://YOUR_VM_EXTERNAL_IP:8081
```

**Note:** Your docker-compose.yaml uses port 8081 (mapped from container's 8080)

**Login:**
- Username: `admin`
- Password: `admin` (or whatever you set in `_AIRFLOW_WWW_USER_PASSWORD`)

---

## 🔒 **Step 6: Secure Access (Recommended)**

### **6.1 Option A: Use Cloud IAP (Identity-Aware Proxy) - Recommended**

```bash
# Enable IAP API
gcloud services enable iap.googleapis.com

# Create firewall rule for IAP
gcloud compute firewall-rules create allow-iap-ssh \
    --allow tcp:22 \
    --source-ranges 35.235.240.0/20 \
    --target-tags allow-iap-ssh

# Add tag to VM
gcloud compute instances add-tags citeconnect-vm \
    --zone=us-central1-a \
    --tags=allow-iap-ssh
```

Then access via IAP tunnel:
```bash
gcloud compute start-iap-tunnel citeconnect-vm 8081 \
    --zone=us-central1-a \
    --local-host-port=localhost:8081
```

Access at: `http://localhost:8081`

### **6.2 Option B: Use SSH Tunnel**

```bash
# Create SSH tunnel
gcloud compute ssh citeconnect-vm \
    --zone=us-central1-a \
    -- -L 8081:localhost:8081 -N
```

Then access at: `http://localhost:8081`

### **6.3 Option C: Restrict Firewall (Less Secure)**

```bash
# Allow only your IP
gcloud compute firewall-rules create allow-airflow-specific \
    --allow tcp:8081 \
    --source-ranges YOUR_IP_ADDRESS/32 \
    --target-tags http-server
```

---

## 🔄 **Step 7: Set Up Auto-Start on Boot**

### **7.1 Create Systemd Service**

```bash
sudo nano /etc/systemd/system/citeconnect.service
```

Add:

```ini
[Unit]
Description=CiteConnect Airflow Pipeline
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/YOUR_USERNAME/citeconnect
ExecStart=/usr/bin/docker-compose up -d
ExecStop=/usr/bin/docker-compose down
User=YOUR_USERNAME
Group=docker

[Install]
WantedBy=multi-user.target
```

Replace `YOUR_USERNAME` with your VM username (run `whoami` to find it).

### **7.2 Enable Service**

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable service
sudo systemctl enable citeconnect.service

# Start service
sudo systemctl start citeconnect.service

# Check status
sudo systemctl status citeconnect.service
```

---

## 💾 **Step 8: Set Up Persistent Storage**

### **8.1 Create Persistent Disk (Optional but Recommended)**

```bash
# Create a persistent disk for data
gcloud compute disks create citeconnect-data \
    --size=100GB \
    --zone=us-central1-a \
    --type=pd-standard

# Attach disk to VM
gcloud compute instances attach-disk citeconnect-vm \
    --disk=citeconnect-data \
    --zone=us-central1-a
```

### **8.2 Format and Mount Disk**

```bash
# SSH into VM
gcloud compute ssh citeconnect-vm --zone=us-central1-a

# Format disk (only first time)
sudo mkfs.ext4 -m 0 -F /dev/sdb

# Create mount point
sudo mkdir -p /mnt/citeconnect-data

# Mount disk
sudo mount -o discard,defaults /dev/sdb /mnt/citeconnect-data

# Make it persistent
echo '/dev/sdb /mnt/citeconnect-data ext4 defaults 1 1' | sudo tee -a /etc/fstab

# Update docker-compose volumes to use this path
# Edit docker-compose.yaml to mount volumes to /mnt/citeconnect-data
```

---

## 🔄 **Step 9: Set Up Automated Deployment**

### **9.1 Create Deployment Script**

Create `scripts/deploy_to_vm.sh` on your local machine:

```bash
#!/bin/bash
# Deploy CiteConnect to GCE VM

set -e

VM_NAME="citeconnect-vm"
ZONE="us-central1-a"
PROJECT_DIR="~/citeconnect"

echo "🚀 Deploying to GCE VM..."

# Copy files to VM
echo "📤 Copying files..."
gcloud compute scp --recurse \
    dags/ src/ services/ utils/ databias/ tests/ configs/ \
    docker-compose.yaml Dockerfile requirements.txt \
    ${VM_NAME}:${PROJECT_DIR} \
    --zone=${ZONE}

# SSH and restart services
echo "🔄 Restarting services..."
gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command="
    cd ${PROJECT_DIR} && \
    docker-compose down && \
    docker-compose pull && \
    docker-compose up -d --build
"

echo "✅ Deployment complete!"
```

Make executable:
```bash
chmod +x scripts/deploy_to_vm.sh
```

### **9.2 Deploy**

```bash
./scripts/deploy_to_vm.sh
```

---

## 📊 **Step 10: Monitor and Maintain**

### **10.1 View Logs**

```bash
# SSH into VM
gcloud compute ssh citeconnect-vm --zone=us-central1-a

# View Docker Compose logs
cd ~/citeconnect
docker-compose logs -f

# View specific service logs
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
```

### **10.2 Check Service Status**

```bash
# Check Docker containers
docker-compose ps

# Check system resources
htop
df -h
free -h
```

### **10.3 Update Code**

```bash
# Pull latest code
cd ~/citeconnect
git pull

# Rebuild and restart
docker-compose down
docker-compose up -d --build
```

---

## 🔧 **Step 11: Troubleshooting**

### **Issue 1: Can't Access Airflow UI**

**Check:**
```bash
# Check if services are running
docker-compose ps

# Check firewall rules
gcloud compute firewall-rules list

# Check if port is listening
sudo netstat -tlnp | grep 8081
```

**Solution:**
- Ensure firewall rule allows port 8081
- Check VM external IP is correct
- Verify services are running: `docker-compose ps`

### **Issue 2: Services Not Starting**

**Check logs:**
```bash
docker-compose logs
```

**Common fixes:**
- Check `.env` file exists and has correct values
- Verify GCP credentials file exists
- Check disk space: `df -h`
- Check memory: `free -h`

### **Issue 3: Out of Disk Space**

**Solution:**
```bash
# Clean up Docker
docker system prune -a --volumes

# Remove old images
docker image prune -a

# Increase disk size (requires VM restart)
gcloud compute disks resize citeconnect-vm \
    --size=100GB \
    --zone=us-central1-a
```

### **Issue 4: Services Keep Restarting**

**Check:**
```bash
# View detailed logs
docker-compose logs --tail=100

# Check system resources
htop
df -h
```

**Common causes:**
- Out of memory
- Disk full
- Configuration errors

---

## 💰 **Cost Estimation**

### **Monthly Costs (Approximate)**

- **VM (e2-standard-4):** ~$100/month
- **Disk (50GB):** ~$8/month
- **Network:** ~$10/month (depends on usage)
- **Total: ~$118/month**

### **Cost Optimization**

1. **Use smaller machine type** for development (e2-standard-2: ~$50/month)
2. **Stop VM when not in use:**
   ```bash
   gcloud compute instances stop citeconnect-vm --zone=us-central1-a
   ```
3. **Use preemptible instances** (60-80% cheaper, but can be terminated)
4. **Resize disk** based on actual needs

---

## 🔐 **Security Best Practices**

1. **Change Default Passwords**
   - Update Airflow admin password
   - Use strong passwords

2. **Restrict Firewall Access**
   - Only allow your IP or use IAP/SSH tunnel

3. **Keep System Updated**
   ```bash
   sudo apt-get update && sudo apt-get upgrade -y
   ```

4. **Use Service Account Keys Securely**
   - Store credentials securely
   - Rotate keys regularly
   - Use least privilege principle

5. **Enable Logging**
   - Monitor access logs
   - Set up alerts for suspicious activity

---

## 📋 **Quick Reference Commands**

```bash
# SSH into VM
gcloud compute ssh citeconnect-vm --zone=us-central1-a

# Start services
cd ~/citeconnect && docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Restart services
docker-compose restart

# Get VM IP
gcloud compute instances describe citeconnect-vm \
    --zone=us-central1-a \
    --format="get(networkInterfaces[0].accessConfigs[0].natIP)"

# Stop VM (to save costs)
gcloud compute instances stop citeconnect-vm --zone=us-central1-a

# Start VM
gcloud compute instances start citeconnect-vm --zone=us-central1-a
```

---

## ✅ **Deployment Checklist**

- [ ] GCP project with billing enabled
- [ ] Compute Engine API enabled
- [ ] VM instance created
- [ ] Docker and Docker Compose installed
- [ ] Repository cloned/copied to VM
- [ ] Environment variables configured (.env file)
- [ ] GCP credentials set up
- [ ] Firewall rules configured
- [ ] Docker Compose services started
- [ ] Airflow UI accessible
- [ ] DAGs visible and working
- [ ] Auto-start service configured (optional)
- [ ] Persistent storage set up (optional)
- [ ] Monitoring configured

---

## 🎯 **Next Steps**

1. **Test your pipeline** end-to-end
2. **Set up monitoring** and alerts
3. **Configure backups** for important data
4. **Set up CI/CD** for automated deployments
5. **Document** your deployment process

---

## 🆘 **Need Help?**

1. Check Docker Compose logs: `docker-compose logs`
2. Check VM logs: `journalctl -u citeconnect`
3. Verify firewall rules in GCP Console
4. Check VM resources (CPU, memory, disk)
5. Review Airflow task logs in UI

---

**Your pipeline is now running in the cloud! 🚀**

