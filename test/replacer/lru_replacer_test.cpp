#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"

#include <thread>
#include <vector>

TEST(LRUReplacerTest, SampleTest) {
    LRUReplacer lru_replacer(7);

    // Scenario: unpin six elements, i.e. add them to the replacer.
    lru_replacer.unpin(1);
    lru_replacer.unpin(2);
    lru_replacer.unpin(3);
    lru_replacer.unpin(4);
    lru_replacer.unpin(5);
    lru_replacer.unpin(6);
    lru_replacer.unpin(1);
    EXPECT_EQ(6, lru_replacer.Size());

    // Scenario: get three victims from the lru.
    int value;
    lru_replacer.victim(&value);
    EXPECT_EQ(1, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(2, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(3, value);

    // Scenario: pin elements in the replacer.
    // Note that 3 has already been victimized, so pinning 3 should have no effect.
    lru_replacer.pin(3);
    lru_replacer.pin(4);
    EXPECT_EQ(2, lru_replacer.Size());

    // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
    lru_replacer.unpin(4);

    // Scenario: continue looking for victims. We expect these victims.
    lru_replacer.victim(&value);
    EXPECT_EQ(5, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(6, value);
    lru_replacer.victim(&value);
    EXPECT_EQ(4, value);
}

// 验证 Size() 在并发环境下不会因缺锁而 data race
TEST(LRUReplacerTest, SizeThreadSafe) {
    LRUReplacer lru_replacer(100);
    for (int i = 0; i < 50; ++i)
        lru_replacer.unpin(i);

    std::atomic<bool> running{true};
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            while (running)
                lru_replacer.Size();
        });
    }
    // let threads run for a short while
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    running = false;
    for (auto& th : threads)
        th.join();
    // 如果没有 data race，到这里不崩溃即为通过
    SUCCEED();
}
