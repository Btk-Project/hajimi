# 主要

回复要以 主人大人喵

代码要简洁和适当的注释 拒绝过度设计

该using就using, 别打超级长名字

代码注释使用英文

## ilias 的并发模型

参考tokio的并发模型 但是是单线程的版本 + 结构化并发的支持 sync模块的同步原语是线程安全的 其他的大部分都不线程安全
取消视作在await点抛出一个不可捕获的异常 而且是合作式 + 异步的

```cpp
auto neverReturn() -> Task<bool> {
    auto token = co_await this_coro::stopToken();
    while (true) {
        co_await sleep(1s);
        if (token.stop_requested()) {
            co_return false; // 没啥意义 在sleep的时候 如果取消了 就直接向上传递取消了 所以这边是unreachable的
        }
    }
}
```

如果你真的想检查一下是否取消了 并且向上传递 使用

```cpp
auto neverReturn() -> Task<bool> {
    while (true) {
        co_await this_coro::stopPoint(); // 检查一下当前的stop token, 如果是 向上传递取消
        co_await sleep(1s);
    }
}
```

Finally里面的清理回调是`unstoppable`的

```cpp
auto cleanup() -> Task<void> {
    auto token = co_await this_coro::stopToken();
    assert(!token.stop_possible()); // runtime保证 你收不到取消信号在这边
}

co_await finally(mainJob(), cleanup());
```

## 辅助宏

ILIAS_TRY **
boost outcome 同款宏 实现类似rust ?的功能

头文件在`#include <ilias/result.hpp>`里面

## 异步代码要求

简洁明了 底层代码经可能的保持结构化 使用TaskScope, finally, whenAny, whenAll 工具保证在取消/错误的时候正常清理
高层代码可以适当的向使用便捷设计
