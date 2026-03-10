# Cloudflare R2 File Manager

`cloudflare-r2-file-manager` is a full-stack web application for managing files and folders stored in Cloudflare R2. It provides both a modern web interface and REST API for performing CRUD operations on files and folders within your R2 bucket.

## ✨ Features

### 🖥️ Web Interface

- **Modern UI**: Clean, responsive web interface built with HTML5, Tailwind CSS, and Alpine.js
- **File Browser**: Navigate folders with breadcrumb navigation
- **Multi-Select**: Advanced file selection with Shift+Click range selection, Ctrl+A, and Esc to clear
- **Drag & Drop**: Intuitive file and folder upload interface
- **Real-time Feedback**: Toast notifications and progress indicators

### 📁 File Management

- **Create** JSON files and folders
- **Read** and **update** file contents
- **Delete** single files or multiple files in batch
- **Upload** multiple files (up to 50 per request) with progress tracking
- **Folder Upload**: Upload entire directory structures while preserving hierarchy
- **Batch Operations**: Delete multiple files in a single request

### 🔧 Advanced Features

- **Natural Sorting**: Proper alphanumeric sorting for files and folders
- **Keyboard Shortcuts**: Full keyboard navigation support
- **Responsive Design**: Mobile-friendly interface
- **Progress Tracking**: Real-time upload progress with visual indicators
- **Error Handling**: Comprehensive error handling with user-friendly messages

### 🌐 API Endpoints

- RESTful API for all file operations
- Pre-signed URL generation for secure file access
- Batch operations for improved performance
- CORS support for web applications

## Prerequisites

- Node.js (v14 or above)
- Cloudflare R2 account and API credentials

## Setup

1. **Clone the repository:**

    ```bash
    https://github.com/YsrajSingh/cloudflare-r2-file-manager.git
    cd cloudflare-r2-file-manager
    ```

2. **Install dependencies:**

    ```bash
    npm install
    ```

3. **Configure R2 accounts and buckets** via the app: open **Settings** in the web UI and add your Cloudflare account (endpoint + API credentials) and buckets. All configuration is stored in SQLite (`data/app.db`); no `.env` file is used.

## Running the Server

To start the server, run:

```bash
npm start
```

The server will be available at `http://localhost:3000`

## Deploying on Render

On Render the filesystem is **ephemeral**: anything written to the app directory (including `data/app.db`) is lost on deploy or restart. To persist the database:

1. **Add a Persistent Disk** to your Web Service in the Render dashboard:
   - **Dashboard** → your service → **Disks** → **Add Disk**
   - Name it (e.g. `data`) and set **Mount Path** to `/data`
   - Save

2. **Set the data directory** so the app uses the disk:
   - **Environment** → add: `DATA_DIR` = `/data`
   - Redeploy

The SQLite database and any config will then be stored on the persistent disk and survive redeploys.

### Render free tier: use Turso (no disk)

On the **free tier** Render does not offer persistent disks. Use **Turso** (free SQLite-compatible database) so your settings persist without a disk:

1. **Create a Turso database** at [turso.tech](https://turso.tech):
   - Sign up, then create a new database (e.g. `r2-manager`).
   - In the database’s **Connect** tab, copy the **URL** and create an **Auth Token**.

2. **Set environment variables** on your Render service:
   - `TURSO_DATABASE_URL` = your Turso URL (e.g. `libsql://r2-manager-yourname.turso.io`)
   - `TURSO_AUTH_TOKEN` = your Turso auth token

3. **Redeploy.** The app will use Turso instead of a local SQLite file; no persistent disk is required.

### Troubleshooting deploy

- **403 when cloning** – If the build log shows `status code: 403` when cloning from GitHub, the repo may be private or Render may have lost access. In Render: **Settings** → **Build & Deploy** → ensure the correct GitHub account/repo is connected and has access. If the deploy still completes (e.g. "Checking out commit..."), the 403 may be from cache download and can be ignored.
- **npm vulnerabilities** – The project is kept updated; run `npm audit fix` if you see audit warnings after install.

## Web Interface

Once the server is running, visit `http://localhost:3000` in your browser to access the web interface:

### Features:
- **File Browser**: Navigate through your folders with breadcrumb navigation
- **Multi-Select**: Select multiple files with Shift+Click range selection, Ctrl+A, or individual clicks
- **File Operations**: Create, upload, delete, and manage files and folders
- **Folder Upload**: Upload entire directory structures while preserving hierarchy
- **Batch Operations**: Delete multiple files efficiently in single requests
- **Progress Tracking**: Real-time upload progress with visual indicators
- **Responsive Design**: Works on desktop and mobile devices
- **Keyboard Shortcuts**: Full keyboard navigation support

### Usage:
1. **Navigation**: Click on folders to navigate, use breadcrumbs to go back
2. **File Selection**: Click files to select, Shift+Click for ranges, Ctrl+A for all
3. **Upload**: Use "Upload" button to select files or drag & drop folders
4. **Delete**: Select files and click "Delete" for batch deletion
5. **Create**: Use "New Folder" or "New File" buttons to create content

## API Endpoints

1. **Create a File**

- Endpoint: `POST /create-file`
- Request Body:

    ```json
    {
        "folder": "folder-name",
        "fileName": "file.json",
        "content": { "key": "value" }
    }
    ```

2. **Delete a File**

- Endpoint: `DELETE /delete-file`
- Request Body (single file):

    ```json
    {
        "folder": "folder-name",
        "fileName": "file.json"
    }
    ```

- Request Body (batch delete multiple files):

    ```json
    {
        "folder": "folder-name",
        "fileNames": ["file1.json", "file2.json", "file3.json"]
    }
    ```

- Request Body (delete all files in folder):

    ```json
    {
        "folder": "folder-name",
        "fileName": "*"
    }
    ```

- Response: `{"message": "Deleted X files successfully"}` or `{"message": "Deleted X files successfully, Y files failed", "deleted": X, "failed": Y, "errors": [...]}`

3. **Update a File**

- Endpoint: `PUT /update-file`
- Request Body:

    ```json
    {
        "folder": "folder-name",
        "fileName": "file.json",
        "content": { "key": "updated-value" }
    }
    ```

4. **Read a File**

- Endpoint: `GET /read-file`
- Query Parameters: `folder=folder-name&fileName=file.json`

5. **List Files in a Folder**

- Endpoint: `GET /list-files`
- Query Parameters: `folder=folder-name`

6. **List All Folders**

- Endpoint: `GET /list-folders`

7. **Duplicate a Folder**

- Endpoint: `POST /duplicate-folder`
- Request Body:

    ```json
    {
        "sourceFolder": "source-folder",
        "targetFolder": "target-folder"
    }
    ```

8. **Rename a Folder**

- Endpoint: `PUT /rename-folder`
- Request Body:

    ```json
    {
        "sourceFolder": "old-folder-name",
        "targetFolder": "new-folder-name"
    }
    ```

9. **Upload Files**

- Endpoint: `POST /upload-files`
- Content-Type: `multipart/form-data`
- Form Data:
  - `files`: Multiple files (up to 50 per request, processed in batches of 10)
  - `folder`: Target folder name (optional, defaults to "uploads")
- Response: `{"message": "Files uploaded successfully", "fileNames": ["file1.jpg", "file2.png"]}`

10. **Get File URLs**

- Endpoint: `GET /get-file-urls`
- Query Parameters: `folder=folder-name&expires=3600` (expires is optional)
- Returns pre-signed URLs for all files in the specified folder

## Contributing

1. Fork the repository.
1. Create a new branch (`git checkout -b feature-name`).
1. Commit your changes (`git commit -am 'Add new feature'`).
1. Push to the branch (`git push origin feature-name`).
1. Create a new pull request.

## License

This project is licensed under the [MIT License](https://github.com/YsrajSingh/cloudflare-r2-file-manager/blob/main/LICENSE).
