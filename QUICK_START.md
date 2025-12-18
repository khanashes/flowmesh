# Quick Start: Test FlowMesh Web UI Locally

## 🐳 Option 1: Docker (Easiest - Works Right Now!)

Since you have Node.js 20.9.0 (needs 20.19+), Docker is the fastest way to test:

```bash
# Build and run (includes Web UI)
cd /Users/eapple/Desktop/Open\ source/FlowMesh
make docker-build
docker-compose up -d

# View logs
docker-compose logs -f
```

**Then open:** http://localhost:8080

Everything is included - no separate frontend/backend setup needed!

To stop:
```bash
docker-compose down
```

## 💻 Option 2: Local Development (Requires Node.js 20.19+)

### Upgrade Node.js First

**Using nvm (recommended):**
```bash
# Install nvm if you don't have it
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash

# Restart terminal or source profile
source ~/.zshrc

# Install and use Node.js 20.19+
nvm install 20.19
nvm use 20.19

# Verify
node --version  # Should show v20.19.0 or higher
```

**Or download from nodejs.org:**
- Visit: https://nodejs.org/
- Download Node.js 20.19+ or 22.12+

### Then Start Services

**Terminal 1 (Backend):**
```bash
cd /Users/eapple/Desktop/Open\ source/FlowMesh
./bin/flowmesh
```

**Terminal 2 (Frontend):**
```bash
cd /Users/eapple/Desktop/Open\ source/FlowMesh/web-ui
npm run dev
```

**Then open:** http://localhost:5173

## 📝 What to Test

1. **Health Page** - Navigate to Health in sidebar
   - Should show green "OK" status
   - Auto-refreshes every 5 seconds

2. **Dashboard** - Main landing page

3. **API Health** - Verify backend:
   ```bash
   curl http://localhost:8080/health
   ```

## 🐛 Troubleshooting

**Docker approach:**
- If build fails, check Docker is running
- If port 8080 is busy: `lsof -i :8080` and kill process

**Local approach:**
- Node.js version must be 20.19+ or 22.12+
- Make sure backend is running before starting frontend
- Check browser console for errors
