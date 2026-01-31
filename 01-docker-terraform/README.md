# Module 1: Docker, Terraform, and Google Cloud Platform (GCP) Setup

Welcome to the first module of the Data Engineering Zoomcamp! In this module, you'll learn how to set up and use Docker, Terraform, and Google Cloud Platform (GCP) for your data engineering projects. This guide provides an overview of the major concepts, step-by-step walkthroughs, and essential terminal commands (using Git Bash).

---

## Table of Contents
1. [Docker Overview & Setup](#docker-overview--setup)
2. [Terraform Overview & Setup](#terraform-overview--setup)
3. [Google Cloud Platform (GCP) Setup](#google-cloud-platform-gcp-setup)
4. [Terminal Commands (Git Bash)](#terminal-commands-git-bash)
5. [Workflow Walkthrough](#workflow-walkthrough)
6. [References](#references)

---

## Docker Overview & Setup
**Docker** is a platform for developing, shipping, and running applications inside containers. Containers package up code and dependencies, ensuring consistency across environments.

### Key Concepts
- **Images**: Blueprints for containers.
- **Containers**: Running instances of images.
- **Dockerfile**: Script to build Docker images.
- **Docker Hub**: Public registry for images.

### Installation
1. Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
2. Verify installation:
	```bash
	docker --version
	docker run hello-world
	```

---

## Terraform Overview & Setup
**Terraform** is an Infrastructure as Code (IaC) tool for provisioning and managing cloud resources.

### Key Concepts
- **Providers**: Plugins for cloud platforms (e.g., GCP).
- **Resources**: Infrastructure components (VMs, storage, etc.).
- **State File**: Tracks infrastructure state.
- **Modules**: Reusable Terraform configurations.

### Installation
1. Download from [Terraform Downloads](https://www.terraform.io/downloads.html).
2. Verify installation:
	```bash
	terraform -v
	```

---

## Google Cloud Platform (GCP) Setup
**GCP** provides cloud computing services for deploying and managing applications.

### Key Concepts
- **Projects**: Isolated workspaces for resources.
- **Service Accounts**: Identities for automation.
- **IAM**: Access management.

### Setup Steps
1. Create a [Google Cloud account](https://cloud.google.com/).
2. Set up a new project.
3. Enable billing and required APIs (e.g., Compute Engine, Storage).
4. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install):
	```bash
	gcloud init
	gcloud auth login
	gcloud config set project <PROJECT_ID>
	```

---

## Terminal Commands (Git Bash)
Common commands for working with Docker, Terraform, and GCP:

### Docker
```bash
docker build -t <image_name> .
docker images
docker run -it <image_name>
docker ps -a
docker stop <container_id>
docker rm <container_id>
```

### Terraform
```bash
terraform init
terraform plan
terraform apply
terraform destroy
```

### GCP (gcloud)
```bash
gcloud auth login
gcloud config set project <PROJECT_ID>
gcloud compute instances list
```

---

## Workflow Walkthrough
1. **Install Docker, Terraform, and Google Cloud SDK** on your machine.
2. **Authenticate with GCP** using `gcloud` and set your project.
3. **Write Terraform configuration files** to define your infrastructure.
4. **Initialize Terraform** in your project directory:
	```bash
	terraform init
	```
5. **Plan and apply infrastructure changes**:
	```bash
	terraform plan
	terraform apply
	```
6. **Build and run Docker containers** for your applications:
	```bash
	docker build -t my_app .
	docker run -p 8080:8080 my_app
	```
7. **Manage resources** using Docker, Terraform, and GCP CLI as needed.

---

## References
- [Docker Documentation](https://docs.docker.com/)
- [Terraform Documentation](https://developer.hashicorp.com/terraform/docs)
- [Google Cloud Documentation](https://cloud.google.com/docs)
