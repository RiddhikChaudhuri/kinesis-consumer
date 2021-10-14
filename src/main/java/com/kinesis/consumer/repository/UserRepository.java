package com.kinesis.consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.kinesis.consumer.dto.User;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
}