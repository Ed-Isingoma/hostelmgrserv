const express = require('express');
const dbScript = require('./dbScript');
const app = express();
const port = 3000;

app.use(express.json())

app.use((req, res, next) => {
	res.set('Content-Security-Policy', "default-src 'self' https://hostelmgr.onrender.com; script-src 'self' 'unsafe-inline';")
	res.set('Cross-Origin-Opener-Policy', "cross-origin")
	res.set('Access-Control-Allow-Origin', "*")
	res.set('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
	res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization')
	next()
})

app.options('*', (req, res) => {
	res.set('Access-Control-Allow-Origin', "*")
	res.set('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
	res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization')
	res.sendStatus(200)
})

app.get('/', (req, res) => {
  res.json({ success: true, message: 'Hello Client'})
})

app.post('/call', async (req, res) => {
  const { funcName, params = [] } = req.body;

  try {
    if (typeof dbScript[funcName] === 'function') {
      const resp = await dbScript[funcName](...params);
      return res.json({ success: true, data: resp });
    } else {
      console.error("Function not found:", funcName)
      return res.json({ success: false, message: 'Function not found:' + funcName})
    }
  } catch (error) {
    console.error(error.message || error);
    console.log('variables:', params);
    return res.json({ success: false, error: error.message || error });
  }
});

app.use((req, res) => {
  res.status(404).json({ success: false, message: 'Endpoint not found' });
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
