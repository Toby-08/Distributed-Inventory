# Step-by-Step Startup Guide

Follow these steps in order to start the Distributed Inventory System with Web UI.

## Prerequisites Check

1. **Python Version**: Make sure you have Python 3.8 or higher
   ```bash
   python --version
   ```

2. **Virtual Environment**: Activate your virtual environment (if using one)
   ```bash
   # Windows
   venv\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

## Step 1: Install Dependencies

Open **Terminal/Command Prompt** and run:

```bash
pip install -r requirements.txt
```

This will install:
- grpcio, grpcio-tools, protobuf
- PyJWT
- google-generativeai
- Flask
- flask-cors

**Wait for installation to complete before proceeding.**

---

## Step 2: Start the LLM Server

Open **Terminal 1** (keep this open):

```bash
python -m llm_server.llm_server
```

**Expected Output:**
```
[LLM] Starting LLM Server...
[LLM] Server running on port 50054...
```

**✅ Keep this terminal open - DO NOT CLOSE**

---

## Step 3: Start the Raft Leader Server

Open **Terminal 2** (new terminal window):

```bash
python -m server_app_leader.app_server leader
```

**Expected Output:**
```
[leader] ==================================================
[leader] Starting Raft Node
[leader] ==================================================
[leader] Node ID:     leader
[leader] Port:        50051
[leader] Server running on port 50051...
```

**✅ Keep this terminal open - DO NOT CLOSE**

---

## Step 4: Start Raft Follower Servers (Optional but Recommended)

### Follower 1 - Terminal 3:

```bash
python -m server_app_leader.app_server follower1
```

**Expected Output:**
```
[follower1] Starting Raft Node
[follower1] Port: 50052
```

### Follower 2 - Terminal 4:

```bash
python -m server_app_leader.app_server follower2
```

**Expected Output:**
```
[follower2] Starting Raft Node
[follower2] Port: 50053
```

**✅ Keep these terminals open - DO NOT CLOSE**

> **Note**: You can run with just the leader, but followers provide redundancy.

---

## Step 5: Start the Flask Web Server

Open **Terminal 5** (new terminal window):

```bash
python web_server.py
```

**Expected Output:**
```
Starting Flask web server on http://localhost:5000
 * Running on http://0.0.0.0:5000
```

**✅ Keep this terminal open - DO NOT CLOSE**

---

## Step 6: Access the Web UI

1. **Open your web browser** (Chrome, Firefox, Edge, etc.)

2. **Navigate to:**
   ```
   http://localhost:5000
   ```

3. **You should see the landing page** with options for:
   - Customer Login/Sign Up
   - Admin Login/Sign Up

---

## Step 7: Test the System

### For Admin Access:

1. Click **"Admin Login"**
2. Use credentials:
   - Username: `admin`
   - Password: `admin123`
3. You'll see the Admin Dashboard with:
   - Add Inventory
   - Update Inventory
   - View Inventory
   - AI Query

### For Customer Access:

1. Click **"Customer Sign Up"**
2. Create a new account (username and password)
3. You'll see the Shopping Page with:
   - Product browsing
   - Shopping cart
   - Purchase functionality

---

## Quick Start (All in One)

If you want to start everything quickly, you can use these commands in separate terminals:

**Terminal 1:**
```bash
python -m llm_server.llm_server
```

**Terminal 2:**
```bash
python -m server_app_leader.app_server leader
```

**Terminal 3:**
```bash
python web_server.py
```

Then open: `http://localhost:5000`

---

## Troubleshooting

### Problem: "Module not found" errors
**Solution:** Make sure you're in the project directory and dependencies are installed:
```bash
pip install -r requirements.txt
```

### Problem: "Port already in use"
**Solution:** 
- Check if another instance is running
- Kill the process using the port
- Or change the port in the code

### Problem: "Cannot connect to server"
**Solution:**
- Make sure all servers are running
- Check that ports 50051, 50052, 50053, 50054, and 5000 are not blocked by firewall
- Verify you're using `localhost` or `127.0.0.1`

### Problem: Web page shows "Connection error"
**Solution:**
- Ensure Flask web server is running (Terminal 5)
- Ensure Raft leader is running (Terminal 2)
- Check browser console for detailed errors (F12)

### Problem: "Invalid credentials" on login
**Solution:**
- Admin: Use `admin` / `admin123`
- Or register a new customer account

---

## Stopping the System

To stop all servers:
1. Go to each terminal window
2. Press `Ctrl + C` in each one
3. Wait for graceful shutdown

---

## Default Credentials

**Admin Users:**
- Username: `admin`, Password: `admin123`
- Username: `user`, Password: `1234`
- Username: `jenil`, Password: `1234`

**Customers:**
- Register new accounts through the signup page

---

## Next Steps

Once everything is running:
1. ✅ Test admin functions (add/update inventory)
2. ✅ Test customer shopping (browse, cart, purchase)
3. ✅ Try AI analysis dashboard
4. ✅ Test on mobile device (responsive design)

Enjoy your Distributed Inventory System! 🎉

