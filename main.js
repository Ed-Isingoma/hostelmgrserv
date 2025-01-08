const express = require('express');
const bodyParser = require('body-parser');

const dbScript = require('./dbScript');

const app = express();
const port = 3000;

app.use(bodyParser.json());

app.post('/call', async (req, res) => {
  const { funcName, params } = req.body;

  try {
    if (typeof dbScript[funcName] === 'function') {
      const resp = await dbScript[funcName](...params);
      return res.json({ success: true, data: resp });
    } else {
      throw new Error('Function not found');
    }
  } catch (error) {
    console.error('Error in function call:', error);
    console.log('variables:', params);
    return res.json({ success: false, error: error.message });
  }
});

app.get('/', (req, res) => {
  return res.json({ msg: 'Server is running'})
})

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
