"use client";

import { UserButton, useUser } from "@clerk/nextjs";
import { useCallback, useRef, useSyncExternalStore } from "react";

import { clerkUserProfileAppearance } from "@/components/app/clerk-appearance";
import { designTokens } from "@/lib/design-tokens";

const subscribe = () => () => {};
const clientSnapshot = () => true;
const serverSnapshot = () => false;

export function AccountMenu() {
  const userMenuRef = useRef<HTMLDivElement | null>(null);
  // Clerk may already be loaded when this subtree hydrates. Keep the first
  // browser render identical to SSR, then mount its client-owned widget.
  const hasHydrated = useSyncExternalStore(subscribe, clientSnapshot, serverSnapshot);
  const clerkUser = useUser();
  const user = hasHydrated ? clerkUser.user : null;
  const isUserLoaded = hasHydrated && clerkUser.isLoaded;
  const primaryEmail = user?.primaryEmailAddress?.emailAddress ?? user?.emailAddresses[0]?.emailAddress;
  const userDisplayName =
    user?.fullName ?? user?.firstName ?? user?.username ?? primaryEmail ?? "Account";
  const userDetail = primaryEmail ?? (isUserLoaded ? "Signed in" : "Loading profile");
  const userInitial = userDisplayName.trim().charAt(0).toUpperCase() || "A";
  const fallback = (
    <span className="practiceUserFallbackAvatar" aria-hidden="true">
      {userInitial}
    </span>
  );

  const openUserMenu = useCallback(() => {
    userMenuRef.current?.querySelector<HTMLButtonElement>(".learnrecurUserButton")?.click();
  }, []);

  return (
    <div ref={userMenuRef} className="practiceUserMenu">
      <div className="practiceUserProfile">
        {hasHydrated ? (
          <UserButton
            fallback={fallback}
            appearance={{
              elements: {
                userButtonAvatarBox: "learnrecurUserAvatar",
                userButtonTrigger: "learnrecurUserButton",
              },
              variables: {
                colorPrimary: designTokens.colorPrimary,
              },
            }}
            userProfileProps={{ appearance: clerkUserProfileAppearance }}
          />
        ) : fallback}
        <button
          type="button"
          className="practiceUserIdentity"
          onClick={openUserMenu}
          aria-label={`Open account menu for ${userDisplayName}`}
        >
          <span className="practiceUserName">{userDisplayName}</span>
          <span className="practiceUserMeta">{userDetail}</span>
        </button>
      </div>
    </div>
  );
}
