package com.kinesis.consumer.service;

import java.util.List;

import com.kinesis.consumer.dto.User;

public interface UserService {

	public List<User> addUsers(List<User> usersList);

}