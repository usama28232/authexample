package users

const GET_ALL_USERS = `select * from users`
const GET_USER_BY_USERNAME = `select * from users where username = $1`
const ADD_USER = `INSERT INTO users(user_id, username, full_name, password, email, created_on, last_login)
VALUES ($1, $2, $3, $4, $5, now(), now())`
const UPDATE_LOGIN_BY_USERNAME = `update users set last_login = now() where username = $1`
const DELETE_USER_BY_USERNAME = `delete from users where username = $1`
