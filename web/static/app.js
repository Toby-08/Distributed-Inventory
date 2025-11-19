// API Base URL
const API_BASE = 'http://localhost:5000/api';

// Get authentication token
function getToken() {
    return localStorage.getItem('token');
}

// API Functions
async function getInventory() {
    try {
        const token = getToken();
        const response = await fetch(`${API_BASE}/inventory`, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        });
        return await response.json();
    } catch (error) {
        return { status: "error", message: error.message };
    }
}


async function addInventory(product, quantity) {
    const token = getToken();
    const response = await fetch(`${API_BASE}/inventory`, {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
            operation: 'add_inventory',
            product,
            quantity
        })
    });
    
    const result = await response.json();
    
    // Auto-refresh inventory after successful add
    if (result.status === 'success') {
        // Wait 300ms for Raft to commit before refreshing
        await new Promise(resolve => setTimeout(resolve, 300));
        await loadInventory();
    }
    
    return result;
}


async function updateInventory(product, quantity) {
    const token = getToken();
    const response = await fetch(`${API_BASE}/inventory`, {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
            operation: 'update_inventory',
            product,
            quantity
        })
    });
    
    const result = await response.json();
    
    // Auto-refresh inventory after successful update
    if (result.status === 'success') {
        // Wait 300ms for Raft to commit before refreshing
        await new Promise(resolve => setTimeout(resolve, 300));
        await loadInventory();
    }
    
    return result;
}


async function purchaseItems(items) {
    const results = [];
    for (const it of items) {
        const r = await updateInventory(it.product, -it.quantity);
        results.push(r);
    }
    
    // Refresh once after all purchases
    await loadInventory();
    
    return { success: true, results };
}


async function queryAI(query) {
    const token = getToken();
    return fetch(`${API_BASE}/query`, {
        method: 'POST',
        headers: { 
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
            query
        })
    }).then(r => r.json());
}


// Login function
async function login(username, password) {
    try {
        const response = await fetch(`${API_BASE}/auth/login`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, password })
        });
        
        const data = await response.json();
        
        if (data.status === 'success') {
            localStorage.setItem('token', data.token);
            localStorage.setItem('username', username);
            return true;
        }
        return false;
    } catch (error) {
        console.error('Login error:', error);
        return false;
    }
}

// Signup function
async function signup(username, password) {
    try {
        const response = await fetch(`${API_BASE}/auth/signup`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, password })
        });
        
        const data = await response.json();
        
        if (data.status === 'success') {
            localStorage.setItem('token', data.token);
            localStorage.setItem('username', username);
            return true;
        }
        return false;
    } catch (error) {
        console.error('Signup error:', error);
        return false;
    }
}

// Load inventory and display
async function loadInventory() {
    const data = await getInventory();
    if (data.status === "success") {
        // Admin table
        const tbody = document.getElementById('inventory-table-body');
        if (tbody) {
            if (data.inventory && data.inventory.length > 0) {
                tbody.innerHTML = data.inventory.map(item => `
                    <tr>
                        <td>${item.product}</td>
                        <td>${item.quantity}</td>
                    </tr>
                `).join('');
            } else {
                tbody.innerHTML =
                    '<tr><td colspan="2" style="text-align: center; padding: 2rem;">No inventory items</td></tr>';
            }
        }

        // Customer list
        const listDiv = document.getElementById('inventory-list');
        if (listDiv) {
            if (!data.inventory || data.inventory.length === 0) {
                listDiv.innerHTML = '<p>No inventory items</p>';
            } else {
                listDiv.innerHTML = data.inventory.map(item => `
                    <div class="inventory-item">
                        <div>
                            <strong>${item.product}</strong>
                            <p>Quantity: ${item.quantity}</p>
                        </div>
                    </div>
                `).join('');
            }
        }
    } else {
        console.error('Failed to load inventory:', data.message);
    }
    return data;
}

// Auto-refresh inventory every 5 seconds (optional - for real-time updates)
let autoRefreshInterval = null;

function startAutoRefresh() {
    // Clear any existing interval
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }
    
    // Refresh every 5 seconds
    autoRefreshInterval = setInterval(() => {
        loadInventory();
    }, 30000);
}

function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// Start auto-refresh when page loads (if on admin/customer page)
if (window.location.pathname.includes('admin.html') || window.location.pathname.includes('customer.html')) {
    startAutoRefresh();
}
