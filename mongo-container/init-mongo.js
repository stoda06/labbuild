db.createUser({
    user: 'labbuild_user',
    pwd: '$$u1QBd6&372#$rF',
    roles: [
      { role: 'readWrite', db: 'labbuild_db' },
      { role: 'dbAdmin', db: 'labbuild_db' }
    ]
  });
  
  db.createCollection('courseconfig');  // Optional: Create a collection in the new database
  