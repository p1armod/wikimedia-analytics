package com.wikimedia.gateway.repository;

import com.wikimedia.gateway.model.BotRatioSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;


@Repository
public interface BotRatioSnapshotRepository extends JpaRepository<BotRatioSnapshot, Long> {

    
    List<BotRatioSnapshot> findByRecordedAtBetweenOrderByRecordedAtAsc(Instant from, Instant to);

    
    List<BotRatioSnapshot> findTop20ByOrderByRecordedAtDesc();

    
    void deleteByRecordedAtBefore(Instant expiryDate);
}
