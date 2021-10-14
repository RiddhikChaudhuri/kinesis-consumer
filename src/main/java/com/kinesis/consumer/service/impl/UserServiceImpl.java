package com.kinesis.consumer.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.kinesis.consumer.dto.User;
import com.kinesis.consumer.repository.UserRepository;
import com.kinesis.consumer.service.UserService;

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private UserRepository userRepository;

  

	@Override
	public List<User> addUsers(List<User> userDetails) {
		return userRepository.saveAll(userDetails);
	}

}