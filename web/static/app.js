// API Base URL
const API_BASE = 'http://localhost:5000/api';

// Get authentication token
function getToken() {
    return localStorage.getItem('token');
}

// API Functions
async function getInventory() {
    try {
        const response = await fetch(`${API_BASE}/inventory`);
        return await response.json();
    } catch (error) {
        return { success: false, message: error.message };
    }
}

async function addInventory(product, quantity) {
    try {
        const response = await fetch(`${API_BASE}/inventory/add`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                token: getToken(),
                product: product,
                quantity: quantity
            })
        });
        return await response.json();
    } catch (error) {
        return { success: false, message: error.message };
    }
}

async function updateInventory(product, quantity) {
    try {
        const response = await fetch(`${API_BASE}/inventory/update`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                token: getToken(),
                product: product,
                quantity: quantity
            })
        });
        return await response.json();
    } catch (error) {
        return { success: false, message: error.message };
    }
}

async function purchaseItems(items) {
    try {
        const response = await fetch(`${API_BASE}/inventory/purchase`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                token: getToken(),
                items: items
            })
        });
        return await response.json();
    } catch (error) {
        return { success: false, message: error.message };
    }
}

async function queryAI(query) {
    try {
        const response = await fetch(`${API_BASE}/ai/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                token: getToken(),
                query: query
            })
        });
        return await response.json();
    } catch (error) {
        return { success: false, message: error.message };
    }
}

// Load inventory and display
async function loadInventory() {
    const data = await getInventory();
    if (data.success) {
        // For admin dashboard table
        const tbody = document.getElementById('inventory-table-body');
        if (tbody) {
            if (data.items.length > 0) {
                tbody.innerHTML = data.items.map(item => `
                    <tr>
                        <td>${item.product}</td>
                        <td>${item.quantity}</td>
                    </tr>
                `).join('');
            } else {
                tbody.innerHTML = '<tr><td colspan="2" style="text-align: center; padding: 2rem;">No inventory items</td></tr>';
            }
        }
        
        // For other pages that use inventory-list
        const listDiv = document.getElementById('inventory-list');
        if (listDiv) {
            if (data.items.length === 0) {
                listDiv.innerHTML = '<p>No inventory items</p>';
            } else {
                listDiv.innerHTML = data.items.map(item => `
                    <div class="inventory-item">
                        <div>
                            <strong>${item.product}</strong>
                            <p>Quantity: ${item.quantity}</p>
                        </div>
                    </div>
                `).join('');
            }
        }
    }
    return data;
}

